from torchvision import models
import torch

alexnet = models.alexnet(pretrained=True)
torch.save(alexnet, './Db/alexnet')
