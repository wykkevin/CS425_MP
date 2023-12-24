from torchvision import models
import torch

resnet = models.resnet50(pretrained=True)
torch.save(resnet, './Db/resnet')
